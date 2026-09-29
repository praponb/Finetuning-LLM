from pyspark.sql import functions as F

result = (
    df_customers
    .select("signup_date")
    .intersect(
        df_stores
        .filter(F.col("sqft").between(7027, 15942))
        .select("opened_date")
    )
)
