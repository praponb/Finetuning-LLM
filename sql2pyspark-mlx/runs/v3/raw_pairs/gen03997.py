from pyspark.sql import functions as F

recent_suppliers = df_suppliers.filter(F.lower(F.trim("supplier_name")) == "eurogoods gmbh")

summary = (
    recent_suppliers
    .groupBy("city")
    .agg(
        F.countDistinct("rating").alias("unique_countrys"),
        F.sum("rating").alias("total_rating"),
    )
)

result = (
    summary
    .select("city", "unique_countrys", "total_rating")
    .orderBy(F.col("total_rating").desc(), "city")
    .limit(20)
)
