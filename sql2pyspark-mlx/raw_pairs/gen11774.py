from pyspark.sql import functions as F

result = (
    df_customers
    .select("segment", "city")
    .intersect(
        df_customers
        .filter(F.upper("name").like("%IA %"))
        .select("segment", "city")
    )
)
