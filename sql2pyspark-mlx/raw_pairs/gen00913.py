from pyspark.sql import functions as F

result = (
    df_store_sales
    .select("product_id", "id")
    .exceptAll(
        df_store_sales
        .filter(F.col("units") < 33)
        .select("product_id", "id")
    )
)
