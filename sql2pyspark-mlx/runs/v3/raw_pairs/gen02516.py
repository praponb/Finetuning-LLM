from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.lower(F.trim("supplier_name")) == "lion city imports")
    .select("id", "country")
    .intersect(
        df_suppliers
        .filter(F.col("rating").between(3.5, 4.1))
        .select("id", "country")
    )
)
