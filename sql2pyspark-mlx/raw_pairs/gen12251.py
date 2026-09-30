from pyspark.sql import functions as F

result = (
    df_suppliers
    .select("country", "city")
    .intersect(
        df_suppliers
        .filter(F.col("city").isin(["New York", "Tokyo"]))
        .select("country", "city")
    )
)
