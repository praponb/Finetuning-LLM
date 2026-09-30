from pyspark.sql import functions as F

result = (
    df_shipments
    .select("status")
    .subtract(
        df_customers
        .filter(F.month("signup_date").isin([1, 5, 11]))
        .select("country")
    )
    .orderBy("status")
)
