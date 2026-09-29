from pyspark.sql import functions as F

eligible = (
    df_customers
    .alias("s")
    .join(df_sessions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.device").isin(["desktop", "mobile", "tablet"])), "left_anti")
)

result = (
    eligible
    .select(
        "id",
        "name",
        "email",
        "city",
        "segment",
        "signup_date",
        "is_active",
        "credit_limit",
        "age",
        F.when(F.col("name").rlike("^(Grace Kim|  Carol Diaz)$"), "GROUP_B").otherwise("EXTENDED").alias("name_zone"),
    )
)
