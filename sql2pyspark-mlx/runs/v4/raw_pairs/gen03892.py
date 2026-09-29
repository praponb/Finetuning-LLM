from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("category", "brand").orderBy("price", "id")
w2 = Window.orderBy("price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy(F.col("cost").desc(), "id")
w5 = Window.partitionBy("brand").orderBy("price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("brand").orderBy("running_price", "id")
w7 = Window.partitionBy("category", "brand")
w8 = Window.orderBy(F.col("cumulative_last_known_cost").desc())
w9 = Window.partitionBy("category").orderBy(F.col("next_cost").desc_nulls_last())
w10 = Window.partitionBy("brand", "category")
w11 = Window.partitionBy("brand")

base = (
    df_products
    .filter((F.col("launch_date") >= F.lit("2024-01-30").cast("date")) & F.col("supplier_id").isin([1, 5, 38]))
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "price",
        "cost",
        "tags",
        F.when(F.col("category").isin(["Fashion", "Home"]), "Tier 1").otherwise("Other").alias("category_group"),
        F.coalesce("brand", F.lit("NONE")).alias("resolved_brand"),
    )
)

enriched = (
    base
    .select(
        "*",
        F.lead("cost", 1).over(w_hist).alias("next_cost"),
        F.first("brand", ignorenulls=True).over(w2).alias("first_known_brand"),
        F.last("cost", ignorenulls=True).over(w3).alias("last_known_cost"),
        F.ntile(10).over(w4).alias("cost_bucket"),
        F.sum("price").over(w5).alias("running_price"),
    )
)

scored = (
    enriched
    .withColumn("running_price_bucket", F.ntile(10).over(w6))
    .withColumn("cumulative_last_known_cost", F.sum("last_known_cost").over(w5))
)

eligible = (
    scored
    .alias("s")
    .join(df_inventory.alias("x"), (F.col("x.product_id") == F.col("s.id")) & (F.col("x.last_restock").between("2023-07-01", "2024-08-01")), "left_anti")
)

kept_keys = (
    eligible
    .select("brand")
    .exceptAll(
        df_products
        .filter(F.col("supplier_id") == 13)
        .select("brand")
    )
)

reconciled = (
    eligible
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.brand") == F.col("k.brand")), "inner")
    .select("s.*")
)

scored_products = (
    reconciled
    .withColumn("cost_share", F.round((F.col("cost") / F.sum("cost").over(w7)), 4))
    .withColumn("cumulative_last_known_cost_percent_rank", F.percent_rank().over(w8))
    .withColumn("next_cost_cume_dist", F.cume_dist().over(w9))
    .withColumn("brand_sum_running_price_bucket", F.sum("running_price_bucket").over(w10))
    .withColumn("cost_share_2", F.round((F.col("cost") / F.sum("cost").over(w11)), 4))
)

result = (
    scored_products
    .select(
        "product_name",
        "category",
        "brand",
        "price",
        "cost",
        "category_group",
        "next_cost",
        "first_known_brand",
        "last_known_cost",
    )
    .orderBy(F.col("cost").desc())
)
