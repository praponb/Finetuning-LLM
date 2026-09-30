from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand", "product_id").orderBy("units_num", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("brand").orderBy("cost", "id").rowsBetween(-2, Window.currentRow)
w3 = Window.orderBy(F.col("cost").desc(), "id")
w4 = Window.partitionBy("brand")
w5 = Window.partitionBy("brand").orderBy(F.col("cost").desc_nulls_first())
w6 = Window.orderBy(F.col("last3_max_units").asc_nulls_first())

filtered = (
    df_store_sales
    .filter(F.col("sale_date") > F.to_date(F.lit("2023-03-02")))
    .select(
        "id",
        "product_id",
        "sale_date",
        "units",
        F.col("units").cast("double").alias("units_num"),
    )
)

joined = (
    filtered
    .alias("a")
    .join(df_products.alias("p"), F.col("a.product_id") == F.col("p.id"), "inner")
    .select("a.*", F.col("p.brand"), F.col("p.product_name"), F.col("p.cost"))
)

enriched = (
    joined
    .select(
        "id",
        "units_num",
        "brand",
        "cost",
        F.sum("units").over(w).alias("last3_sum_units"),
        F.avg("units").over(w).alias("last3_avg_units"),
        F.max("units").over(w).alias("last3_max_units"),
    )
)

metrics = (
    enriched
    .select(
        "*",
        F.avg("last3_max_units").over(w2).alias("last3_avg_last3_max_units"),
        F.min("last3_max_units").over(w2).alias("last3_min_last3_max_units"),
        F.max("last3_max_units").over(w2).alias("last3_max_last3_max_units"),
        F.ntile(10).over(w3).alias("cost_bucket"),
    )
)

kept_keys = (
    metrics
    .select("id")
    .subtract(
        df_store_sales
        .filter(F.col("units") >= 23)
        .select("id")
    )
)

kept_rows = (
    metrics
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

enriched_store_sales = (
    kept_rows
    .withColumn("last3_max_units_share", F.round((F.col("last3_max_units") / F.sum("last3_max_units").over(w4)), 4))
    .withColumn("last3_sum_units_share", F.round((F.col("last3_sum_units") / F.sum("last3_sum_units").over(w4)), 4))
    .withColumn("brand_max_last3_min_last3_max_units", F.max("last3_min_last3_max_units").over(w4))
    .withColumn("cost_cume_dist", F.cume_dist().over(w5))
    .withColumn("last3_max_units_rank", F.rank().over(w6))
)

base = (
    enriched_store_sales
    .filter(F.col("brand").isNotNull() & F.col("cost_cume_dist").between(0.23, 0.65))
    .withColumn("last3_sum_units_level", F.when(F.col("last3_sum_units") >= 220, "High").when(F.col("last3_sum_units") >= 28, "Medium").otherwise("Low"))
    .withColumn("resolved_brand", F.coalesce("brand", F.lit("UNKNOWN")))
)

result = (
    base
    .select("brand", "last3_max_last3_max_units", "units_num", "last3_avg_units")
    .unpivot(
        ids=["brand"],
        values=["last3_max_last3_max_units", "units_num", "last3_avg_units"],
        variableColumnName="measure",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
