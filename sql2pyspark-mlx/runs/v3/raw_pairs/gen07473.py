from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("sqft").between(7812, 15040))
    .select("opened_date", "sqft")
    .unionByName(
        df_stores
        .filter(F.col("opened_date") <= "2025-03-01")
        .select("opened_date", "sqft")
    )
    .distinct()
)
