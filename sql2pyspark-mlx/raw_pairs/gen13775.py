from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("sqft").desc(), "id")
w2 = Window.partitionBy("product_id").orderBy("sale_date", "id")
w3 = Window.partitionBy("store_id").orderBy(F.col("sqft").desc_nulls_last())
w4 = Window.partitionBy(F.month("sale_date"), "store_id").orderBy(F.col("units").desc(), "id")
w5 = Window.partitionBy("product_id", "store_id").orderBy("units", "id").rowsBetween(-3, Window.currentRow)
w6 = Window.partitionBy("product_id").orderBy("prev_units", "id").rowsBetween(-2, -1)
w7 = Window.partitionBy("product_id", "store_id").orderBy("sale_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_store_sales
    .alias("s")
    .join(df_stores.alias("b"), F.col("s.store_id") == F.col("b.id"), "left")
    .select(
        "s.id",
        "s.store_id",
        "s.product_id",
        "s.sale_date",
        "s.units",
        F.col("b.sqft"),
    )
)

scored = (
    enriched
    .filter(F.col("store_id").isin([1, 3, 5, 6]))
    .withColumn("sqft_bucket", F.ntile(4).over(w))
    .withColumn("prev_units", F.lag("units", 1).over(w2))
    .withColumn("sqft_percent_rank", F.percent_rank().over(w3))
    .withColumn("units_bucket", F.ntile(4).over(w4))
    .withColumn("units_avg_3", F.avg("units").over(w5))
)

cleaned = (
    scored
    .filter(F.col("sqft_percent_rank") <= 0.6)
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units",
        "sqft_bucket",
        "prev_units",
        "units_avg_3",
        F.least("prev_units", "units_bucket").alias("least_prev_units_units_bucket"),
    )
)

enriched_store_sales = (
    cleaned
    .select(
        "*",
        F.avg("sqft_bucket").over(w6).alias("sqft_bucket_avg_2_prior"),
        F.sum("prev_units").over(w7).alias("cumulative_prev_units"),
    )
)

result = (
    enriched_store_sales
    .alias("e")
    .join(df_stores.alias("s"), F.col("e.store_id") == F.col("s.id"), "inner")
    .cube(F.col("s.region").alias("region"))
    .agg(
        F.count("*").alias("rows_total"),
        F.avg(F.col("e.least_prev_units_units_bucket")).alias("avg_least_prev_units_units_bucket"),
        F.min(F.col("e.units")).alias("min_units"),
    )
    .orderBy(F.col("region").desc())
)
