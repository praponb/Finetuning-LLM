from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("sale_date_period", "id")
w2 = Window.partitionBy("product_id").orderBy("sale_date_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_store_sales
    .filter(F.col("store_id").isin([1, 2, 5]))
    .withColumn("units_rounded", F.round("units", 0))
)

valid_store_sales = (
    valid
    .filter((F.col("sale_date") >= F.to_date(F.lit("2023-08-21"))) & (F.col("units") <= 22))
    .withColumn("sale_date_period", F.date_trunc("MONTH", "sale_date"))
    .withColumn("least_units_units_rounded", F.least("units", "units_rounded"))
)

valid_store_sales_2 = (
    valid_store_sales
    .filter(F.col("sale_date") >= F.lit("2023-03-31").cast("date"))
    .withColumn("units_rounded_int", F.col("units_rounded").cast("int"))
)

with_gaps = (
    valid_store_sales_2
    .withColumn("is_new_burst", F.when(F.col("sale_date_period") - F.lag("sale_date_period").over(w) > F.expr("INTERVAL 60 DAYS"), 1).otherwise(0))
)

islands = with_gaps.withColumn("session_no", F.sum("is_new_burst").over(w2))

result = (
    islands
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units",
        "revenue",
        "units_rounded_int",
        "is_new_burst",
    )
    .orderBy(F.col("product_id").desc_nulls_last())
)
