from pyspark.sql import functions as F

result = (
    df_suppliers
    .select("id", "country")
    .intersectAll(
        df_suppliers
        .filter(F.col("city").isNull())
        .select("id", "country")
    )
)
