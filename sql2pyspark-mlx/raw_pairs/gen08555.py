from pyspark.sql import functions as F

result = (
    df_web_events
    .select("page")
    .intersect(df_products.select("brand"))
    .orderBy("page")
)
