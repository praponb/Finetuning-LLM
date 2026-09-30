from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("units") < 39)
    .select(
        "id",
        "sale_date",
        "store_id",
        F.date_trunc("WEEK", "sale_date").alias("sale_date_period"),
        F.date_add("sale_date", 78).alias("sale_date_plus_78d"),
    )
)
