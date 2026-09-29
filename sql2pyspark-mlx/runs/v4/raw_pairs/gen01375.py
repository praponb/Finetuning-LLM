from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.orderBy("sale_date_plus_22d", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy(F.col("first_known_units").asc_nulls_last())
w4 = Window.orderBy("units", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("first_known_units", "id")

filtered = (
    df_store_sales
    .filter((F.col("sale_date") > F.to_date(F.lit("2023-06-22"))) & (F.col("sale_date") < "2024-08-01"))
    .select(
        "id",
        "sale_date",
        "units",
        "revenue",
        F.date_add("sale_date", 22).alias("sale_date_plus_22d"),
    )
)

enriched = (
    filtered
    .withColumn("first_known_units", F.first("units", ignorenulls=True).over(w_recent))
    .withColumn("cumulative_units", F.sum("units").over(w2))
)

filtered_store_sales = (
    enriched
    .filter(F.col("first_known_units").between(16, 27))
    .select(
        "*",
        F.round((F.col("cumulative_units") * 0.9), 2).alias("cumulative_units_adjusted"),
    )
)

enriched_store_sales = (
    filtered_store_sales
    .withColumn("first_known_units_percent_rank", F.percent_rank().over(w3))
    .withColumn("last_known_cumulative_units", F.last("cumulative_units", ignorenulls=True).over(w4))
    .withColumn("first_known_units_bucket", F.ntile(3).over(w5))
)

result = (
    enriched_store_sales
    .select(
        "id",
        "units",
        "first_known_units",
        "cumulative_units",
        "cumulative_units_adjusted",
        "first_known_units_percent_rank",
        "last_known_cumulative_units",
        "first_known_units_bucket",
    )
    .orderBy(F.col("units").desc_nulls_last())
)
