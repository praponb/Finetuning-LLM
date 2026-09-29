from pyspark.sql import functions as F

result = (
    df_payments
    .select("status")
    .distinct()
    .crossJoin(df_shipments.select("carrier").distinct())
)
