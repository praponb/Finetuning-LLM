from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.units").desc(), F.col("c.id").desc())

latest_match = (
    df_stores
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.region",
        "s.opened_date",
        "s.sqft",
        F.col("c.revenue").alias("store_sale_revenue"),
    )
)

result = (
    latest_match
    .withColumn("activity_month", F.date_trunc("MONTH", "opened_date"))
    .groupingSets(
        [["region", "activity_month"], ["region"], []],
        "region",
        "activity_month",
    )
    .agg(
        F.stddev_samp("sqft").alias("sqft_stddev"),
        F.round(F.sum("sqft") / F.nullif(F.count(F.when(F.col("region") == "East", 1)), F.lit(0)), 2).alias("sqft_per_east"),
        F.grouping("region").alias("gid"),
    )
    .orderBy(F.col("region").desc_nulls_first())
)
