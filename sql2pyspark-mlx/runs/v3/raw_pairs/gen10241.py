from pyspark.sql import functions as F

result = (
    df_customers
    .filter(~F.col("name").like("%Oliv%") & F.col("age").between(40, 48) & F.upper("name").like("%ACK%"))
    .select(
        "id",
        "age",
        "email",
        "segment",
        F.when(F.col("is_active"), "Yes").otherwise("No").alias("is_active_label"),
    )
    .orderBy(F.col("segment").desc(), "age", "id")
    .limit(3)
)
