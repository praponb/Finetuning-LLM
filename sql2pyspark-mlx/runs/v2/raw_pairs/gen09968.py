from pyspark.sql import functions as F

result = (
    df_customers
    .select("city")
    .unionByName(
        df_stores
        .filter(F.month("opened_date").isin([5, 8, 10]))
        .select("city")
    )
    .distinct()
)
