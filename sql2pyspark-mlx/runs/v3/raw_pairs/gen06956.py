from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("name").like("Emm%"))
    .select(
        "id",
        "name",
        "is_active",
        "signup_date",
        F.when(F.col("country").isin(["UK", "SG"]), "Group A").otherwise("Other").alias("country_group"),
    )
)
