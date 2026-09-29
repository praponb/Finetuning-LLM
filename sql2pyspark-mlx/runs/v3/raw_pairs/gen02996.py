from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("age").between(56, 57) & ~F.col("is_active"))
    .select(
        "id",
        "segment",
        "email",
        F.when(F.col("country").isin(["DE", "SG"]), "Group A").otherwise("Other").alias("country_group"),
    )
)
