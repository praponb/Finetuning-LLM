from pyspark.sql import functions as F

result = (
    df_store_sales
    .select(
        "id",
        "units",
        "store_id",
        F.weekofyear("sale_date").alias("sale_date_weekofyear"),
    )
    .orderBy(F.col("units").desc())
)
