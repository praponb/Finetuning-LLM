from pyspark.sql import functions as F

valid_listeners = (
    df_listeners
    .filter((F.col("signup_date").isNotNull()) & (~F.col("plan").isin("free", "family")))
    .select(
        "id",
        "username",
        "plan",
        "country",
        "signup_date",
        "interests",
    )
)

listener_side = (
    valid_listeners
    .filter(F.year(F.col("signup_date")) == 2024)
    .select("username")
)

listener_rows = (
    df_listeners
    .filter(F.col("plan") != "family")
    .select("plan")
)

result = listener_side.exceptAll(listener_rows)
