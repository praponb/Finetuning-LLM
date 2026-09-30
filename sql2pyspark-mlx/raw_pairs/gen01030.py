from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("sale_date_month_end", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("product_id").orderBy(F.col("units").desc_nulls_last())
w3 = Window.partitionBy("product_id").orderBy(F.col("sale_date_month_end").desc(), "id")
w4 = Window.partitionBy("store_id", "product_id").orderBy(F.col("sale_date").desc_nulls_last())
w_grp = Window.partitionBy(F.year("sale_date")).orderBy("first_known_revenue_int", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp_2 = Window.orderBy("units_rank", "id").rowsBetween(-6, -1)
w7 = Window.partitionBy("store_id").orderBy("units_rank", "id").rowsBetween(-3, -1)

valid = (
    df_store_sales
    .filter((F.col("sale_date") >= F.to_date(F.lit("2023-02-25"))) & (F.col("sale_date") < "2023-04-13"))
    .withColumn("sale_date_month_end", F.last_day("sale_date"))
)

with_windows = (
    valid
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units",
        "revenue",
        F.first("revenue", ignorenulls=True).over(w).alias("first_known_revenue"),
        F.rank().over(w2).alias("units_rank"),
        F.row_number().over(w3).alias("seq_no"),
    )
)

filtered = (
    with_windows
    .filter(F.col("revenue") <= 2900)
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units",
        "units_rank",
        F.col("first_known_revenue").cast("int").alias("first_known_revenue_int"),
    )
)

ranked = (
    filtered
    .withColumn("dedup_rank", F.rank().over(w4))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

enriched = (
    ranked
    .withColumn("first_known_units", F.first("units", ignorenulls=True).over(w_grp))
    .withColumn("first_known_revenue_int_avg_6_prior", F.avg("first_known_revenue_int").over(w_grp_2))
    .withColumn("first_known_revenue_int_avg_3_prior", F.avg("first_known_revenue_int").over(w7))
)

result = (
    enriched
    .select(
        "id",
        "store_id",
        "product_id",
        "units",
        "units_rank",
        "first_known_revenue_int",
        "first_known_units",
        "first_known_revenue_int_avg_6_prior",
        "first_known_revenue_int_avg_3_prior",
    )
    .orderBy("units_rank")
)
