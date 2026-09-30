from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.length("supplier_name") >= 14)
    .select("supplier_name")
    .intersect(df_subscriptions.select("status"))
    .orderBy("supplier_name")
)
