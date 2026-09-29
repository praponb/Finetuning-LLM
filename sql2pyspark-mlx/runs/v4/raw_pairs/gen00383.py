from pyspark.sql import functions as F

result = (
    df_products
    .select("id", "brand", "price", "cost")
    .unpivot(["id", "brand"], ["price", "cost"], "metric_name", "reading")
    .filter(F.col("reading").isNotNull())
)
