from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("units").between(22, 29))
    .select(
        "id",
        "product_id",
        "units",
        F.date_add("sale_date", 24).alias("sale_date_plus_24d"),
    )
    .orderBy("product_id", "sale_date_plus_24d", "id")
    .limit(10)
)
