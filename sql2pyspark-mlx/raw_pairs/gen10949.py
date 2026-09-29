from pyspark.sql import functions as F

result = (
    df_suppliers
    .select("country")
    .subtract(
        df_suppliers
        .filter(F.col("country").isin(["TH", "SG", "JP"]))
        .select("country")
    )
)
