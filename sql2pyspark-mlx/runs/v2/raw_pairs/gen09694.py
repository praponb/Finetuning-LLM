from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.month("signup_date").isin([1, 3, 12]))
    .select("signup_date", "age")
    .subtract(
        df_customers
        .filter(~F.col("segment").isin(["SMB", "Consumer"]))
        .select("signup_date", "age")
    )
    .orderBy("signup_date")
)
