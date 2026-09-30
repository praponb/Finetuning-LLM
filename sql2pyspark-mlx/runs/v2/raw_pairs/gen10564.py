from pyspark.sql import functions as F

result = (
    df_orders
    .select("status")
    .subtract(
        df_subscriptions
        .filter(F.col("end_date") > "2023-09-05")
        .select("status")
    )
)
