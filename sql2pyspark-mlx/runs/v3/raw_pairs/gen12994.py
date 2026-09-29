from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.year("start_date") >= 2025)
    .select(
        "id",
        "end_date",
        "start_date",
        F.add_months("end_date", 1).alias("end_date_plus_months"),
    )
)
