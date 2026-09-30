from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("sale_date").between("2023-12-08", "2024-04-05"))
    .select(
        "id",
        "units",
        "store_id",
        "sale_date",
        F.date_add("sale_date", 42).alias("sale_date_plus_42d"),
        F.when(F.col("revenue") >= 3500, "High").when(F.col("revenue") >= 2000, "Medium").otherwise("Low").alias("revenue_level"),
    )
    .orderBy("sale_date")
)
