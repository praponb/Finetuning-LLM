from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.year("shipped_date") == 2023)
    .select("delivered_date")
    .union(
        df_products
        .filter(F.length("product_name") >= 7)
        .select("launch_date")
    )
)
