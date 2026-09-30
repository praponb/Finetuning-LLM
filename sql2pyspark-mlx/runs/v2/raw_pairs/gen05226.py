from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("email").like("%@startup.io"))
    .select("city")
    .unionByName(
        df_suppliers
        .filter(F.col("city").isin(["Bangkok", "New York", "London"]))
        .select("city")
    )
    .orderBy(F.col("city").desc())
)
