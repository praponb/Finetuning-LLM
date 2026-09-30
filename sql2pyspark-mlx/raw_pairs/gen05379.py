from pyspark.sql import functions as F

result = (
    df_products
    .filter(~F.col("brand").isin(["Umbrella", "Acme", "Initech"]) & F.col("supplier_id").isNull())
    .select("brand", "launch_date", "price")
    .orderBy("launch_date")
)
