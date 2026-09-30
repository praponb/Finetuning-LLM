from pyspark.sql import functions as F

result = (
    df_payments
    .select("status")
    .subtract(df_subscriptions.filter(F.year("end_date") >= 2023).select("status"))
)
