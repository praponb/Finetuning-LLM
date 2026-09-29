from pyspark.sql import functions as F

result = (
    df_suppliers
    .select("id", "city")
    .subtract(
        df_suppliers
        .filter(F.col("rating") < 3.9)
        .select("id", "city")
    )
)
