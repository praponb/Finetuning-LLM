from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "level",
        "status_code",
        F.explode_outer("context").alias("key", "attr_value"),
    )
)

result = (
    flattened
    .select(
        "id",
        "service",
        "host",
        "status_code",
        F.when(F.col("service").rlike("^(auth|billing)$"), "GROUP_B").when(F.col("service").rlike("^(search|checkout)$"), "EXTENDED").otherwise("GROUP_A").alias("service_zone"),
    )
    .orderBy(F.col("host").desc())
)
