from pyspark.sql import functions as F

listener_rows = (
    df_listeners
    .select(
        F.col("country").alias("category"),
        F.col("birth_year").alias("entity_id"),
        "plan",
    )
)

listener_rows_2 = (
    df_listeners
    .filter((F.col("country") != "TH") & (F.col("birth_year") > 1975))
    .select(
        F.col("plan").alias("category"),
        F.col("birth_year").alias("entity_id"),
        F.col("country").alias("plan"),
    )
)

result = (
    listener_rows
    .unionByName(listener_rows_2)
    .orderBy(F.col("category").desc())
)
