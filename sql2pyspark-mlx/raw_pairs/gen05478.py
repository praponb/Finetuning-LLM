from pyspark.sql import functions as F

result = (
    df_suppliers
    .select("country", "city")
    .subtract(
        df_suppliers
        .filter(F.col("city") != "New York")
        .select("country", "city")
    )
)
