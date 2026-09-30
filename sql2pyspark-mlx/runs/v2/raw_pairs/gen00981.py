from pyspark.sql import functions as F

base = (
    df_products
    .filter(F.lower(F.trim("product_name")) == "usb-c hub")
    .withColumn("product_name_length", F.length("product_name"))
)

agg = (
    base
    .groupBy("brand")
    .agg(
        F.sort_array(F.collect_set("category")).alias("category_list"),
        F.count(F.when(F.col("category") == "Home", 1)).alias("home_count"),
    )
)

result = agg.select("brand", "category_list", "home_count")
