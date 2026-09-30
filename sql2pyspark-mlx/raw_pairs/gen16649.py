from pyspark.sql import functions as F

result = (
    df_products
    .select("supplier_id")
    .intersect(
        df_products
        .filter(~F.col("brand").isin(["Initech", "Acme", "Globex"]))
        .select("supplier_id")
    )
)
