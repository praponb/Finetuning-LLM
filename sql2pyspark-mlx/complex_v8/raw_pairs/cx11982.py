from pyspark.sql import functions as F

listener_side = (
    df_listeners
    .filter((F.col("country") == "UK") & (F.col("signup_date") >= F.lit("2024-08-02").cast("date")))
    .select(
        F.col("birth_year").alias("ref_id"),
        F.col("country").alias("name"),
        F.col("signup_date").alias("day"),
        F.lit("listener").alias("record_type"),
    )
)

listener_rows = (
    df_listeners
    .filter(F.year(F.col("signup_date")) == 2024)
    .select(
        "id",
        "country",
        "signup_date",
        F.lit("listener_2").alias("record_type"),
    )
)

combined = (
    listener_side
    .union(listener_rows)
    .distinct()
)

result = (
    combined
    .groupBy("record_type")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("ref_id").alias("total_ref_id"),
        F.max("day").alias("latest_day"),
    )
    .orderBy("record_type")
)
