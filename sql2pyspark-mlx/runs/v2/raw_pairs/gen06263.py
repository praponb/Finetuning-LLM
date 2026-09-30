from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.upper(F.trim("supplier_name")) == "EUROGOODS GMBH") & F.col("country").isin(["JP", "TH"]))
    .select("supplier_name", F.col("rating").cast("int").alias("rating_int"))
    .orderBy(F.col("rating_int").desc())
)
