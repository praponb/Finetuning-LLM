from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("city").isin(["Tokyo", "Singapore"]))
    .select(F.lit("customer").alias("origin"), "city", "country")
    .unionByName(
        df_suppliers
        .filter(F.col("supplier_name").like("%Toky%"))
        .select(F.lit("supplier").alias("origin"), "city", "country")
    )
)
