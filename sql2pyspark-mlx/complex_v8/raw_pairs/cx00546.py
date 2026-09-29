from pyspark.sql import functions as F

listener_side = df_listeners.select(F.lit("listener").alias("record_type"), "country", "signup_date")

listener_rows = (
    df_listeners
    .select(
        F.lit("listener_2").alias("record_type"),
        F.col("plan").alias("country"),
        "signup_date",
    )
)

result = listener_side.unionByName(listener_rows)
