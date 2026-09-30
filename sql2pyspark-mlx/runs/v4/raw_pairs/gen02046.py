from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy(F.col("launch_date_period").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w2 = Window.partitionBy("category")
w_ordered = Window.partitionBy("supplier_id", "brand")
w4 = Window.partitionBy("brand", "category")
w5 = Window.orderBy(F.col("cost").asc_nulls_last())

filtered = (
    df_products
    .filter(F.size("tags") > 2)
    .select(
        "*",
        F.date_trunc("WEEK", "launch_date").alias("launch_date_period"),
        F.coalesce("brand", F.lit("NONE")).alias("brand_clean"),
    )
)

kept_keys = (
    filtered
    .select("supplier_id", "category")
    .intersectAll(
        df_products
        .filter(F.col("launch_date") > "2023-07-01")
        .select("supplier_id", "category")
    )
)

reconciled = (
    filtered
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.supplier_id") == F.col("k.supplier_id")) & (F.col("s.category") == F.col("k.category")), "inner")
    .select("s.*")
)

enriched = (
    reconciled
    .select(
        "*",
        F.avg("price").over(w).alias("price_avg_30d"),
        F.sum("price").over(w2).alias("category_sum_price"),
        F.corr("stock_qty", "cost").over(w_ordered).alias("corr_stock_qty_cost"),
        F.stddev_samp("cost").over(w4).alias("brand_stddev_samp_cost"),
        F.dense_rank().over(w5).alias("cost_dense_rank"),
    )
)

stats = (
    enriched
    .withColumn("cost_tier", F.when(F.col("cost") >= 620, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["category", "cost_tier"], ["category"], []],
        "category",
        "cost_tier",
    )
    .agg(
        F.count_if(F.col("brand") == "Initech").alias("initech_events"),
        F.sum(F.when(F.col("category") == "Sports", F.col("cost"))).alias("sports_total_cost"),
        F.max(F.when(F.col("brand") == "Stark", F.col("price"))).alias("stark_max_price"),
    )
    .filter(F.col("stark_max_price") >= 0)
)

result = (
    stats
    .select(
        "category",
        "initech_events",
        "stark_max_price",
        F.when(F.col("category").rlike("^(Fashion|Home)$"), "EXTENDED").when(F.col("category").rlike("^(Electronics|Sports)$"), "PRIORITY").otherwise("OTHER_ZONE").alias("category_zone"),
    )
    .orderBy(F.col("category").desc())
)
