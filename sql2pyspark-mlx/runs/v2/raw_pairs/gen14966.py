from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("stock_qty") != 190) & F.col("category").isin(["Grocery", "Books", "Home"]))
    .groupBy("brand")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("category"))).alias("category_values"),
    )
    .orderBy("brand", "category_values")
    .limit(5)
)
