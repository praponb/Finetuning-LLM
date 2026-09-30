from pyspark.sql import functions as F

valid_suppliers = df_suppliers.filter(F.col("country").isin(["UK", "US"]))

suppliers_stats = (
    valid_suppliers
    .groupBy("country")
    .agg(F.countDistinct("rating").alias("unique_citys"))
)

result = suppliers_stats.orderBy("unique_citys")
