from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("revenue", "id")
w2 = Window.partitionBy(F.month("sale_date")).orderBy("units", "id")
w3 = Window.partitionBy("product_id").orderBy(F.col("next_units").asc_nulls_last(), "id")

base = (
    df_store_sales
    .filter(F.col("product_id").isin([2, 10]))
    .withColumn("least_units_revenue", F.least("units", "revenue"))
)

filtered = (
    base
    .filter(F.col("sale_date") >= F.lit("2023-04-26").cast("date"))
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units",
        "revenue",
        F.last_day("sale_date").alias("sale_date_month_end"),
    )
)

scored = (
    filtered
    .select(
        "id",
        "store_id",
        "product_id",
        "units",
        "sale_date_month_end",
        F.lead("revenue").over(w).alias("next_revenue"),
        F.lag("units").over(w).alias("prev_units"),
        F.lead("units").over(w).alias("next_units"),
        F.row_number().over(w).alias("row_seq"),
        F.row_number().over(w2).alias("seq_no"),
    )
)

latest = (
    scored
    .withColumn("latest_rank", F.row_number().over(w3))
    .filter(F.col("latest_rank") == 1)
)

result = (
    latest
    .alias("e")
    .join(df_stores.alias("d"), F.col("e.store_id") == F.col("d.id"), "inner")
    .groupBy(F.col("d.region").alias("region"))
    .agg(
        F.count("*").alias("rows_total"),
        F.min(F.col("e.next_revenue")).alias("min_next_revenue"),
    )
    .orderBy(F.col("region").asc_nulls_last())
)
