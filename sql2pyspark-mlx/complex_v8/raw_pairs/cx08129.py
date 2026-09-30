from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("city").orderBy(F.col("opened_on").desc(), F.col("id").desc())

prepared = (
    df_warehouses
    .filter(F.year(F.col("opened_on")) == 2024)
    .select(
        "id",
        "wh_code",
        "city",
        "capacity",
        "opened_on",
        F.substring(F.trim(F.col("city")), 1, 4).alias("city_prefix"),
        F.abs(F.col("capacity") - 11506).alias("capacity_distance"),
    )
)

unique_rows = (
    prepared
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

totals = (
    unique_rows
    .groupBy(F.date_trunc("month", "opened_on").alias("opened_on_month"), "city")
    .agg(
        F.sum("capacity").alias("total_capacity"),
        F.round(F.avg("capacity"), 2).alias("avg_capacity"),
        F.sum("capacity_distance").alias("total_capacity_distance"),
    )
    .filter(F.col("total_capacity") > 4912)
)

result = (
    totals
    .filter(F.col("total_capacity_distance") >= 30000)
    .select("opened_on_month", "city", "total_capacity_distance")
    .orderBy(F.col("total_capacity_distance").asc_nulls_last())
)
