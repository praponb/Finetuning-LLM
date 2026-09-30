from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("email").like("%@GMAIL.COM"))
    .select("country")
    .intersect(
        df_suppliers
        .filter(F.col("supplier_name").like("To%"))
        .select("country")
    )
)
