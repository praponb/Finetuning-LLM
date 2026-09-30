from pyspark.sql import functions as F

result = (
    df_suppliers
    .select("country")
    .subtract(
        df_suppliers
        .filter(F.col("supplier_name").like("Sia%"))
        .select("country")
    )
)
