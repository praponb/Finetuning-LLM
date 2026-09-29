from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.to_date("paid_at") >= "2024-03-01")
    .select("status")
    .subtract(df_products.select("product_name"))
)
