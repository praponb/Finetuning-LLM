from pyspark.sql import functions as F

result = (
    df_suppliers
    .select("id")
    .subtract(
        df_suppliers
        .filter(F.col("rating") > 4.0)
        .select("id")
    )
)
