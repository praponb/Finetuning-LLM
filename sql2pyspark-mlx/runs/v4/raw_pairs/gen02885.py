from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("units") != 20) & (F.col("revenue").between(2300, 3700) | F.col("store_id").isin([2, 3, 4])))
    .select(
        "id",
        "product_id",
        F.datediff(F.current_date(), "sale_date").alias("days_since_sale_date"),
        F.date_add("sale_date", 76).alias("sale_date_plus_76d"),
    )
    .orderBy(F.col("id").desc())
)
