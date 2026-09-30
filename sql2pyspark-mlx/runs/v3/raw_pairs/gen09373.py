from pyspark.sql import functions as F

result = (
    df_shipments
    .select("delivered_date")
    .union(
        df_subscriptions
        .filter(F.year("end_date") == 2025)
        .select("start_date")
    )
)
