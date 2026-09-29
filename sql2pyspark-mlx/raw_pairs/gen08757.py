from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("rating") < 2.9) & F.col("supplier_name").like("Si%"))
    .agg(F.sort_array(F.collect_set("country")).alias("country_list"))
)
