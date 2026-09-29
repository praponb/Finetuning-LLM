from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("revenue") < 3000)
    .select("sale_date", "product_id")
    .subtract(
        df_store_sales
        .filter(F.col("revenue") > 3900)
        .select("sale_date", "product_id")
    )
)
