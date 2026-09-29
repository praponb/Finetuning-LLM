from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("plan") == "standard") & (F.col("end_date") <= "2024-02-26"))
)
