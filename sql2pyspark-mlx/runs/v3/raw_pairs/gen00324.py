from pyspark.sql import functions as F

supplier_totals = (
    df_products
    .groupBy("supplier_id")
    .agg(
        F.countDistinct("stock_qty").alias("unique_suppliers"),
        F.round(F.stddev("price"), 2).alias("stddev_price"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_totals.alias("st"), F.col("s.id") == F.col("st.supplier_id"), "inner")
    .filter(F.col("st.unique_suppliers") >= 10)
    .select(
        F.col("s.id"),
        F.col("s.country"),
        F.col("s.supplier_name"),
        F.col("st.unique_suppliers"),
        F.col("st.stddev_price"),
    )
    .orderBy(F.col("id").desc(), F.col("supplier_name").desc())
    .limit(10)
)
