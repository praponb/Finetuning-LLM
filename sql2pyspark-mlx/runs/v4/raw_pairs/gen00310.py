from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("sale_date").desc(), "id")

deduped = (
    df_store_sales
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

result = (
    deduped
    .withColumn("revenue_tier", F.when(F.col("revenue") >= 3600, "HIGH").otherwise("LOW"))
    .withColumn("activity_month", F.date_trunc("MONTH", "sale_date"))
    .groupingSets(
        [["revenue_tier", "activity_month"], ["revenue_tier"]],
        "revenue_tier",
        "activity_month",
    )
    .agg(
        F.bit_and("units").alias("units_bit_and"),
        F.percentile_approx("revenue", [0.25, 0.5, 0.75]).alias("revenue_quartiles"),
        F.round(F.avg("revenue"), 2).alias("avg_revenue"),
    )
    .orderBy(F.col("revenue_tier").desc())
)
