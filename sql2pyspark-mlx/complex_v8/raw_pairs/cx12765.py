from pyspark.sql import functions as F

listener_side = (
    df_listeners
    .filter(F.col("birth_year") > 1966)
    .select(
        F.col("plan").alias("name"),
        "signup_date",
        F.col("country").alias("name_2"),
    )
)

listener_rows = (
    df_listeners
    .filter((F.size(F.col("interests")) >= 2) & (F.col("username").isNotNull()))
    .select(
        F.col("country").alias("name"),
        "signup_date",
        F.col("plan").alias("name_2"),
    )
)

result = (
    listener_side
    .union(listener_rows)
    .distinct()
)
