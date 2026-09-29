from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(~((F.col("product_id") == 8) & ((F.col("units") > 37) | (F.col("sale_date") <= "2023-09-01"))))
    .select(
        "sale_date",
        "units",
        F.date_trunc("MONTH", "sale_date").alias("sale_date_period"),
    )
    .orderBy(F.col("sale_date").desc(), "sale_date_period")
)
