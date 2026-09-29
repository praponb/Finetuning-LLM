from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("units") <= 13)
    .select("sale_date")
    .subtract(
        df_subscriptions
        .filter(F.col("end_date") > "2025-05-12")
        .select("end_date")
    )
)
