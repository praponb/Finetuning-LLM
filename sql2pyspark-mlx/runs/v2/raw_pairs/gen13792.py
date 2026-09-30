from pyspark.sql import functions as F

filtered_suppliers = (
    df_suppliers
    .filter(F.col("city").isin(["Tokyo", "Berlin", "Bangkok"]) & (F.length("supplier_name") >= 14))
    .withColumn("rating_int", F.col("rating").cast("int"))
)

suppliers_stats = (
    filtered_suppliers
    .groupBy("country")
    .agg(F.sort_array(F.collect_set("country")).alias("country_list"))
)

result = suppliers_stats.orderBy("country")
